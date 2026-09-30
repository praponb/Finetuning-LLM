from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") >= 45)
    .select("city", "country", F.lit("customer").alias("source"))
    .unionByName(
        df_suppliers
        .filter(F.col("city").isin(["Tokyo", "Singapore"]))
        .select("city", "country", F.lit("supplier").alias("source"))
    )
)
