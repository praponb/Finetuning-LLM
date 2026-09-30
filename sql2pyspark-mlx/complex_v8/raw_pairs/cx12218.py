from pyspark.sql import functions as F

tariff_rows = df_tariff_windows.select(F.col("id").alias("entity_id"), "tariff")

tariff_rows_2 = (
    df_tariff_windows
    .filter(F.month(F.col("valid_to")).isin(1, 3))
    .select(F.col("id").alias("entity_id"), "tariff")
)

result = tariff_rows.exceptAll(tariff_rows_2)
