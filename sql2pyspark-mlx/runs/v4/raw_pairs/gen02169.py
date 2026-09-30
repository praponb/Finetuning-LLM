from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("started_at", "id")
w_grp = Window.orderBy("started_at", "id")

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-03-26 08:00:00"))) & F.col("customer_id").isNotNull())
    .withColumn("device_upper", F.upper("device"))
)

scored = (
    cleaned
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w_part))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_part))
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    deduped
    .select(
        "*",
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.row_number().over(w_grp).alias("seq_no_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "device_upper",
        "first_known_country",
        "last_known_browser",
        "seq_no",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "CORE").when(F.col("device").rlike("^(tablet)$"), "GROUP_B").otherwise("PRIORITY").alias("device_zone"),
    )
    .orderBy(F.col("device_zone").asc_nulls_last(), "id")
)
