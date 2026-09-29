from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city", F.lit("customer").alias("source"))
    .unionByName(
        df_suppliers
        .filter(F.col("city").isin(["Chiang Mai", "New York", "London"]))
        .select("country", "city", F.lit("supplier").alias("source"))
    )
    .distinct()
)
