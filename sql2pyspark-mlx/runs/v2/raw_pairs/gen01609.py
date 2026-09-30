from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "country",
        F.concat("country", F.lit(" "), "browser").alias("country_browser"),
        F.when(F.col("device").isin(["desktop", "mobile"]), "Tier 1").otherwise("Other").alias("device_group"),
    )
)
