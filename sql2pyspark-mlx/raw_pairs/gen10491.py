from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() | (F.col("country") != "UK"))
    .select(
        "id",
        "device",
        F.when(F.col("browser").isin(["Chrome", "Edge"]), "Group A").otherwise("Other").alias("browser_group"),
    )
)
