from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["SG", "JP"]))
    .select(
        "id",
        "country",
        "customer_id",
        F.initcap(F.trim("device")).alias("device_title"),
        F.initcap(F.trim("device")).alias("device_title_2"),
    )
    .orderBy(F.col("device_title").desc())
)
