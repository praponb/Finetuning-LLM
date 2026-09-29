from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").asc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity_scaled", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("row_count").desc())

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-08-22")))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 211, "High").when(F.col("quantity") >= 110, "Medium").otherwise("Low"))
    .withColumn("last_restock_period", F.date_trunc("WEEK", "last_restock"))
)

base = (
    filtered
    .filter(F.col("last_restock") < "2025-02-01")
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity_tier",
        "last_restock_period",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_scaled"),
        F.weekofyear("last_restock").alias("last_restock_weekofyear"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity_tier",
        "last_restock_period",
        "quantity_scaled",
        "dedup_rank",
        F.lag("last_restock_weekofyear").over(w2).alias("prev_last_restock_weekofyear"),
        F.lag("quantity_scaled").over(w2).alias("prev_quantity_scaled"),
        F.lead("quantity_scaled").over(w2).alias("next_quantity_scaled"),
        F.row_number().over(w2).alias("row_seq"),
        F.count("last_restock_weekofyear").over(w3).alias("last_restock_weekofyear_count_7d"),
    )
)

result = (
    enriched
    .groupBy(
        "product_id",
        F.date_trunc("month", "last_restock").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum("next_quantity_scaled").alias("total_next_quantity_scaled"),
        F.min("last_restock_period").alias("first_seen"),
    )
    .withColumn("row_count_rank", F.rank().over(w4))
)
