from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_voltage_a = Window.partitionBy("site_id", "quality").orderBy("voltage_adj", "id")
w_all_by_kwh = Window.orderBy(F.col("kwh").asc_nulls_last(), "id")

base = (
    df_meter_readings
    .filter((F.col("kwh") >= 45.0) & (F.col("voltage") >= 222.1))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 0.25, 2).alias("voltage_adj"),
    )
)

windowed = (
    base
    .select(
        "id",
        "quality",
        F.row_number().over(w_site_id_quality_by_voltage_a).alias("voltage_adj_seq"),
        F.rank().over(w_site_id_quality_by_voltage_a).alias("voltage_adj_rank"),
        (F.col("voltage_adj") - F.lag("voltage_adj").over(w_all_by_kwh)).alias("voltage_adj_change"),
    )
)

result = (
    windowed
    .select(
        "id",
        F.col("voltage_adj_rank").alias("voltage_adj_rank_v"),
        F.col("voltage_adj_seq").alias("m_voltage_adj_seq"),
    )
    .unpivot(
        ids=["id"],
        values=["voltage_adj_rank_v", "m_voltage_adj_seq"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
