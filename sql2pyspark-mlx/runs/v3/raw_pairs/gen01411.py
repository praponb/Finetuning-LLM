from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "dept_name",
        F.when(F.col("location").isin(["Chiang Mai", "Tokyo"]), "Priority").otherwise("Other").alias("location_group"),
    )
    .orderBy("location_group")
)
