from pyspark.sql import functions as F

filtered = (
    df_warehouses
    .filter((~F.col("city").isin("Kuala Lumpur", "Bangkok")) & (F.length(F.trim(F.col("wh_code"))) >= 4))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

warehouse_side = filtered.select("city", "opened_on", F.lit("warehouse").alias("record_type"))

warehouse_side_2 = (
    df_warehouses
    .filter((F.lower(F.col("wh_code")).like("%nx%")) & (F.col("capacity") >= 4912))
    .select("city", "opened_on", F.lit("warehouse_2").alias("record_type"))
)

result = (
    warehouse_side
    .union(warehouse_side_2)
    .distinct()
    .orderBy("opened_on", "record_type")
)
