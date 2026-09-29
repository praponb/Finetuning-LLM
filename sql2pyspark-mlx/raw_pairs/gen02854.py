from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", F.lit("customer").alias("record_type"))
    .unionByName(
        df_suppliers
        .filter(F.col("rating") < 3.8)
        .select("country", F.lit("supplier").alias("record_type"))
    )
    .distinct()
    .orderBy("country")
)
