from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "opened_date")).orderBy(F.col("opened_date").desc(), "id")
w2 = Window.orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-24").cast("date")) & (F.col("region").eqNullSafe(F.lit("Central"))))
    .select(
        "id",
        "store_name",
        "opened_date",
        F.when(F.col("sqft") >= 12628, "High").when(F.col("sqft") >= 4435, "Medium").otherwise("Low").alias("sqft_level"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.opened_date")), "left_anti")
)

scored = (
    kept
    .filter(F.month("opened_date").isin([2, 6, 12]))
    .select(
        "id",
        "store_name",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.event_seq", "s.row_seq", F.col("c.units"))
)

valid = (
    latest_match
    .filter(F.col("row_seq") > 8)
    .withColumn("row_seq_whole", F.floor("row_seq"))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
)

result = (
    valid
    .withColumn("units_tier", F.when(F.col("units") >= 41, "LARGE").otherwise("SMALL"))
    .withColumn("units_filled_tier", F.when(F.col("units_filled") >= 60, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["units_tier", "units_filled_tier"], ["units_tier"], []],
        "units_tier",
        "units_filled_tier",
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.avg("row_seq_whole"), 2).alias("avg_row_seq_whole"),
        F.sum("event_seq").alias("total_event_seq"),
    )
    .orderBy(F.col("total_event_seq").desc())
)
