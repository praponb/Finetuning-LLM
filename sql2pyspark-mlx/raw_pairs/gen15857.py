from pyspark.sql import functions as F

result = (
    df_departments
    .select(F.col("dept_name").alias("label"))
    .unionByName(df_shipments.select(F.col("carrier").alias("label")))
    .distinct()
    .orderBy(F.col("label").desc())
)
