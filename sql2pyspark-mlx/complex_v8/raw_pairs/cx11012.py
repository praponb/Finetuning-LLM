from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter(~F.col("tariff").isin("TOU", "EV"))
    .select(
        F.col("id").alias("qty"),
        F.col("rate").alias("value"),
        "tariff",
        F.lit("tariff").alias("src"),
    )
)

tariff_rows = (
    df_tariff_windows
    .select(
        F.col("id").alias("qty"),
        F.col("rate").alias("value"),
        "tariff",
        F.lit("tariff_2").alias("src"),
    )
)

result = tariff_side.unionByName(tariff_rows)
