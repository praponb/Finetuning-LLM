from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date", F.lit("product").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("opened_date") > "2023-03-01")
        .select(F.col("opened_date").alias("launch_date"), F.lit("store").alias("source"))
    )
    .orderBy("launch_date")
)
