from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat_ws("-", "browser", "country").alias("browser_country_key"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("resolved_country"),
    )
)

result = (
    base
    .filter(F.col("browser").isin(["Safari", "Firefox", "Edge"]))
    .select(
        "id",
        "device",
        "country",
        "browser_country_key",
        "resolved_country",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country"),
    )
)
