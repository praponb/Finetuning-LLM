from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

result = (
    base_readings
    .select("id", F.col("kwh").alias("m_kwh"), F.col("voltage").alias("m_voltage"))
    .unpivot(
        ids=["id"],
        values=["m_kwh", "m_voltage"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
