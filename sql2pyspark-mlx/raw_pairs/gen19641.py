from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock_plus_months").desc(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-04-14").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "quantity", "rn")
)

valid = (
    latest
    .filter(F.col("quantity").isNotNull())
    .withColumn("quantity_minus_rn", (F.col("quantity") - F.col("rn")))
    .withColumn("quantity_minus_rn_2", (F.col("quantity") - F.col("rn")))
)

result = (
    valid
    .groupBy(
        F.when(F.col("quantity") >= 235, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("quantity_minus_rn"), 2).alias("avg_quantity_minus_rn"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("quantity_minus_rn"), 2).alias("avg_quantity_minus_rn_2"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy("avg_quantity_minus_rn_2")
)
