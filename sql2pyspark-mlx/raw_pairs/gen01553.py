from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("revenue").desc_nulls_last(), "id")
w_running = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest_match = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.store_sale_sale_date")), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged_stores = (
    ranked
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.channel").isin(["mobile", "web"])), "left_anti")
)

scored = (
    not_flagged_stores
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "revenue",
        F.max("revenue").over(w_running).alias("running_max_revenue"),
        F.min("revenue").over(w_running).alias("running_min_revenue"),
        F.count("revenue").over(w_running).alias("running_count_revenue"),
        F.avg("revenue").over(w_running).alias("running_avg_revenue"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "revenue",
        "running_max_revenue",
        "running_min_revenue",
        "running_count_revenue",
        "running_avg_revenue",
    )
)
