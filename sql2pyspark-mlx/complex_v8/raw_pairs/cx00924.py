from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_quality_group_quality_by_vol = Window.partitionBy("quality_group", "quality").orderBy(F.col("voltage_filled_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_group_quality_by_vol_2 = Window.partitionBy("quality_group", "quality").orderBy(F.col("voltage_filled_adj").desc())

cleaned_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 216.8)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

scored = (
    cleaned_readings
    .select(
        "*",
        F.round(F.col("voltage_filled") * 0.25, 2).alias("voltage_filled_adj"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

sequenced = (
    scored
    .select(
        "id",
        "site_id",
        "voltage",
        "quality_prefix",
        "voltage_filled",
        F.min("kwh").over(w_quality).alias("quality_min_kwh"),
        F.last("voltage_filled", ignorenulls=True).over(w_quality_group_quality_by_vol).alias("last_known_voltage_filled"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.rank().over(w_quality_group_quality_by_vol_2).alias("voltage_filled_adj_rank"),
    )
)

result = (
    sequenced
    .select("id", "rows_in_quality", "voltage_filled_adj_rank")
    .unpivot(
        ids=["id"],
        values=["rows_in_quality", "voltage_filled_adj_rank"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .orderBy("id", "field")
)
