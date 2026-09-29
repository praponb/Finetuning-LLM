from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "region").orderBy("sqft", "id")
w2 = Window.orderBy(F.col("sqft").asc_nulls_first())
w3 = Window.partitionBy("store_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("sqft", "id")
w6 = Window.orderBy(F.col("sqft").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-07-23").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        F.col("d.region"),
        F.col("d.sqft"),
        F.col("d.store_name"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    deduped
    .filter(F.col("store_id").isin([1, 6]))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w2))
    .withColumn("running_sqft", F.sum("sqft").over(w3))
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w4))
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w5))
    .withColumn("sqft_bucket", F.ntile(3).over(w6))
)

result = (
    with_windows
    .select(
        "id",
        "region",
        "sqft",
        "store_name",
        "running_sqft",
        "last_known_region",
        "prev_sqft",
        "sqft_bucket",
        F.when(F.col("store_name").rlike("^(Old Town|Downtown Express)$"), "GROUP_A").when(F.col("store_name").rlike("^(Siam Flagship|Airport Kiosk)$"), "EXTENDED").otherwise("PRIORITY").alias("store_name_zone"),
    )
    .orderBy(F.col("sqft").asc_nulls_last())
)
