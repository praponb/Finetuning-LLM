from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("move_type", "operator").orderBy(F.col("qty").desc(), F.col("id").desc())

cleaned = df_stock_moves.filter((F.col("sku_id").isNotNull()) & (F.col("unit_price") <= 329.7))

latest_per_key = (
    cleaned
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    latest_per_key
    .filter(~F.col("move_type").isin("ADJUST", "OUTBOUND"))
    .select(
        "*",
        F.when(F.col("qty") >= 249, "HIGH").when(F.col("qty") >= 145, "MEDIUM").otherwise("LOW").alias("qty_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.abs(F.col("unit_price") - 200.7).alias("unit_price_distance"),
    )
)

result = (
    scored
    .groupBy("move_type")
    .pivot("operator", ["op_chan", "op_bee", "op_eve", "op_dew"])
    .agg(F.max("qty"))
    .orderBy("move_type")
)
