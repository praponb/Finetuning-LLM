from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Singapore", "Bangkok"]))
    .select("country", F.lit("customer").alias("source"))
    .unionByName(
        df_suppliers
        .filter(F.col("rating") < 2.2)
        .select("country", F.lit("supplier").alias("source"))
    )
)
