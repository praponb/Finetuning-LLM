from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .select(F.lit("customer").alias("origin"), "city", "country")
    .unionByName(
        df_suppliers
        .select(F.lit("supplier").alias("origin"), "city", "country")
    )
    .distinct()
)
