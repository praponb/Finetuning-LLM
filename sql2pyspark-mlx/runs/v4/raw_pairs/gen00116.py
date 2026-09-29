from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")

enriched = (
    df_device_readings
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "first_known_temp_c",
        "row_seq",
        F.when(F.col("device_code").rlike("^(DV-01|DV-03)$"), "GROUP_B").when(F.col("device_code").rlike("^(DV-02|DV-04)$"), "EXTENDED").otherwise("CORE").alias("device_code_zone"),
    )
    .orderBy(F.col("device_code_zone").desc_nulls_first())
)
