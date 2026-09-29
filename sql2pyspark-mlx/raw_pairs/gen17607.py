from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "country").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-09-14 05:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("device")).alias("device_title"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.row_number().over(w).alias("event_seq"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
