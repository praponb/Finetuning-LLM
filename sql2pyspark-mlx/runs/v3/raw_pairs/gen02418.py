from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(9, 15))
    .select(
        "browser",
        "customer_id",
        "started_at",
        F.when(F.col("country").isin(["DE", "UK"]), "Group A").otherwise("Other").alias("country_group"),
    )
    .orderBy("customer_id")
)
