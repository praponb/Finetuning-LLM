from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "device").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w_recent = Window.orderBy(F.col("started_at").desc(), "id")
w4 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .filter(F.col("customer_id").isin([2, 19, 37]))
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.last("browser", ignorenulls=True).over(w4).alias("last_known_browser"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.avg(F.when(F.col("browser") == "Firefox", F.col("row_seq"))).alias("firefox_avg_row_seq"),
        F.round(F.avg("seq_no_2"), 2).alias("avg_seq_no_2"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("avg_seq_no_2").desc_nulls_last())
)
