from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("cost") <= 360) & F.col("cost").between(420, 520)) | F.col("brand").isNotNull())
    .groupBy("category", "brand", F.year("launch_date").alias("launch_year"))
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") < 400)
    .orderBy(F.col("initech_count").desc())
)
