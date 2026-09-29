from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.orderBy(F.col("opened_date").desc(), "id")
w_recent = Window.partitionBy(F.date_trunc("week", "opened_date")).orderBy(F.col("revenue").desc(), "id")
w5 = Window.orderBy("store_sale_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest_match = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.opened_date",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue"),
    )
)

latest_match_stores = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.store_sale_sale_date",
        "s.revenue",
        F.col("c.quantity"),
    )
)

eligible = (
    latest_match_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.store_sale_sale_date")), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") > "2023-07-01"), "left_anti")
)

kept_stores = (
    kept
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.status") != "RETURNED"), "left_anti")
)

not_flagged = (
    kept_stores
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.store_sale_sale_date")), "left_anti")
)

result = (
    not_flagged
    .filter(F.month("store_sale_sale_date").isin([5, 9, 10]))
    .select(
        "id",
        "opened_date",
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(4).over(w_recent).alias("revenue_bucket"),
        F.last("revenue", ignorenulls=True).over(w5).alias("last_known_revenue"),
    )
)
