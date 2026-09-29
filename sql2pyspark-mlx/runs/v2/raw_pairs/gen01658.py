from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~((F.col("country") == "TH") & F.hour("started_at").between(11, 17)))
    .select(
        "id",
        "device",
        "started_at",
        F.initcap(F.trim("device")).alias("device_title"),
    )
    .orderBy(F.col("started_at").desc(), F.col("id").desc())
    .limit(10)
)
