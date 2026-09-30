from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.lit("customer").alias("record_type"), F.col("country").alias("label"))
    .unionByName(
        df_products
        .filter(F.col("supplier_id").isin([26, 28]))
        .select(F.lit("product").alias("record_type"), F.col("brand").alias("label"))
    )
    .distinct()
    .orderBy("record_type")
)
