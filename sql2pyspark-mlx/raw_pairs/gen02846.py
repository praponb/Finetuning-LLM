from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.lit("customer").alias("kind"), "country", "city")
    .unionByName(
        df_suppliers
        .select(F.lit("supplier").alias("kind"), "country", "city")
    )
    .orderBy("country")
)
