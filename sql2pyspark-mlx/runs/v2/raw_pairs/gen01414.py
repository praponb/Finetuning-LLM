from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE"]) & (F.col("plan") == "basic") & (F.year("end_date") == 2025))
    .select(
        "id",
        "end_date",
        F.date_format("end_date", "yyyy-MM").alias("end_date_label"),
        F.when(F.col("status").isin(["EXPIRED", "PAUSED"]), "Group A").otherwise("Other").alias("status_group"),
    )
    .orderBy("end_date")
)
