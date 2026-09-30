from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", "country", F.lit("customer").alias("source"))
    .unionByName(
        df_suppliers
        .select("city", "country", F.lit("supplier").alias("source"))
    )
    .distinct()
)
