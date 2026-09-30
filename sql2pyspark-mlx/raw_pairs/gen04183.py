from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        F.when(F.col("location").isin(["Chiang Mai", "New York"]), "Priority").otherwise("Other").alias("location_group"),
    )
)
