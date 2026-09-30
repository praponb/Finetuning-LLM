from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.orderBy("inventory_quantity", "id")
w4 = Window.orderBy("opened_date", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("inventory_quantity_avg_6_prior", "id")
w6 = Window.orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units") >= 23), "left_anti")
)

ranked = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "region", "opened_date", "sqft")
)

latest_match = (
    ranked
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

with_windows = (
    latest_match
    .filter(F.col("inventory_last_restock") >= "2024-04-01")
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("inventory_quantity_avg_6_prior", F.avg("inventory_quantity").over(w4))
)

result = (
    with_windows
    .select(
        "id",
        "inventory_last_restock",
        "seq_no",
        "inventory_quantity_avg_6_prior",
        F.row_number().over(w5).alias("row_seq"),
        F.last("seq_no", ignorenulls=True).over(w6).alias("last_known_seq_no"),
    )
)
