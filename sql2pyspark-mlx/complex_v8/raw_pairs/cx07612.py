from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_date_year = Window.partitionBy(F.year("read_at_date"))
w_read_at_date_month_site_id_b = Window.partitionBy(F.date_trunc("month", "read_at_date"), "site_id").orderBy("read_at", "id")
w_site_id_by_read_at_date_ties = Window.partitionBy("site_id").orderBy(F.col("read_at_date").desc())
w_site_id_by_read_at_date = Window.partitionBy("site_id").orderBy(F.col("read_at_date").desc(), "id")

recent_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) | (F.col("kwh") >= 20.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

metrics = (
    recent_readings
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_date_year), F.lit(0)), 4).alias("voltage_share_of_read_at_date_year"),
        F.row_number().over(w_read_at_date_month_site_id_b).alias("seq_no"),
        F.rank().over(w_site_id_by_read_at_date_ties).alias("read_at_date_rank"),
        F.dense_rank().over(w_site_id_by_read_at_date).alias("read_at_date_dense_rank"),
        F.lead("voltage").over(w_read_at_date_month_site_id_b).alias("next_voltage"),
    )
)

result = (
    metrics
    .groupingSets(
        [["seq_no", "read_at_date_rank", "read_at_date_dense_rank"], ["seq_no"], []],
        "seq_no", "read_at_date_rank", "read_at_date_dense_rank",
    )
    .agg(
        F.grouping("seq_no").alias("is_total_seq_no"),
        F.max("read_at_date").alias("last_read_at_date"),
        F.sum("read_at_date_dense_rank").alias("total_read_at_date_dense_rank"),
        F.max("read_at").alias("last_read_at"),
    )
    .orderBy(
        F.col("seq_no").asc_nulls_last(),
        F.col("read_at_date_rank").asc_nulls_last(),
        F.col("read_at_date_dense_rank").asc_nulls_last(),
    )
)
