from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.col("created_at") >= "2024-12-01")
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("weight_kg") >= 4.1, "PREMIUM").otherwise("STANDARD").alias("weight_kg_tier"),
        F.when(F.col("num_items") >= 3, "PREMIUM").otherwise("STANDARD").alias("num_items_tier"),
    )
    .agg(
        F.round(F.avg("num_items"), 2).alias("avg_num_items"),
        F.max("created_at").alias("last_created_at"),
        F.max("created_at").alias("last_seen"),
    )
    .filter(F.col("avg_num_items") >= 0)
    .orderBy("weight_kg_tier")
)
