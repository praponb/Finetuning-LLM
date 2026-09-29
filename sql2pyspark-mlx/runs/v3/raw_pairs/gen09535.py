from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city", F.lit("customer").alias("source"))
    .unionByName(
        df_suppliers
        .select("country", "city", F.lit("supplier").alias("source"))
    )
)
