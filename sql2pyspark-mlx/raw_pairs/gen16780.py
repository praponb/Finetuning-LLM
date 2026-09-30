from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("carrier").alias("label"), F.lit("shipment").alias("source"))
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isNull())
        .select(F.col("dept_name").alias("label"), F.lit("department").alias("source"))
    )
    .distinct()
)
