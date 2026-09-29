from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select("city", F.lit("customer").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("opened_date") > "2023-09-01")
        .select("city", F.lit("store").alias("source"))
    )
    .distinct()
)
