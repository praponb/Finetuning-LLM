from pyspark.sql import functions as F

tariff_side = df_tariff_windows.select("tariff")

tariff_rows = (
    df_tariff_windows
    .filter((F.col("tariff") != "TOU") & (F.to_date(F.col("valid_to")) < F.lit("2025-04-05").cast("date")))
    .select("tariff")
)

result = tariff_side.subtract(tariff_rows)
