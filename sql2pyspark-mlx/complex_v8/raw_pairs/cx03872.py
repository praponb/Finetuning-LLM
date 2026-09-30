from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_adj_prior_1 = Window.orderBy(F.col("voltage_adj").desc(), "id").rowsBetween(-1, -1)
w_site_id_read_at_year = Window.partitionBy("site_id", F.year("read_at"))

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

ranked = (
    cleaned
    .withColumn("quality_len_avg_1_prior", F.round(F.avg("quality_len").over(w_all_by_voltage_adj_prior_1), 2))
    .withColumn("kwh_vs_site_id_read_at_year_avg", F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_read_at_year), 2))
)

result = (
    ranked
    .select(
        "id",
        "voltage",
        "quality_len_avg_1_prior",
        "kwh_vs_site_id_read_at_year_avg",
        "kwh",
    )
    .unpivot(
        ids=["id"],
        values=["voltage", "quality_len_avg_1_prior", "kwh_vs_site_id_read_at_year_avg", "kwh"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
