from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w_grp = Window.orderBy("started_at", "id")
w_ordered = Window.partitionBy("browser", "country").orderBy("started_at", "id")

valid = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.to_date("started_at") >= "2023-04-01"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

result = (
    valid
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w_part).alias("last_known_browser"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w_grp).alias("event_seq_2"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)
