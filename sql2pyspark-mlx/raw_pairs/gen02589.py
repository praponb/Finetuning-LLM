from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(5700, 11200))
    .select(F.lit("customer").alias("origin"), "city")
    .unionByName(
        df_stores
        .filter(F.col("city") == "Bangkok")
        .select(F.lit("store").alias("origin"), "city")
    )
    .orderBy("city")
)
