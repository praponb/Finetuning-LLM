from pyspark.sql import functions as F

tariff_rows = df_tariff_windows.select("id", "valid_to")

tariff_side = df_tariff_windows.select("id", "valid_to")

result = (
    tariff_rows
    .union(tariff_side)
    .distinct()
)
