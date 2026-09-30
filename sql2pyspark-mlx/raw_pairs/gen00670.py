from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w_ordered = Window.orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("store_id") == 5)
    .select(
        "id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)

enriched = (
    cleaned
    .withColumn("quantity_bucket", F.ntile(10).over(w))
    .withColumn("quantity_avg_3", F.avg("quantity").over(w_ordered))
)

base = (
    enriched
    .filter(F.col("last_restock") > F.lit("2023-07-20").cast("date"))
    .select("*", F.floor("quantity").alias("quantity_whole"))
)

valid = (
    base
    .filter(F.col("quantity").isNotNull())
    .withColumn("quantity_plus_quantity_avg_3", (F.col("quantity") + F.col("quantity_avg_3")))
)

report = (
    valid
    .select(
        "id",
        "quantity",
        "last_restock",
        "quantity_bucket",
        "quantity_avg_3",
        "quantity_plus_quantity_avg_3",
    )
    .orderBy(F.col("quantity").desc_nulls_last(), F.col("last_restock").desc())
)

result = (
    report
    .select(
        "quantity",
        "last_restock",
        "quantity_bucket",
        "quantity_avg_3",
        "quantity_plus_quantity_avg_3",
    )
)
