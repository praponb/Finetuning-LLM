from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("is_lost").desc_nulls_last(), "id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-07-14").cast("date")) & (F.year("shipped_date") >= 2024))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.add_months("shipped_date", 3).alias("shipped_date_plus_months"),
    )
)

cleaned = (
    filtered
    .filter(~F.col("carrier").eqNullSafe(F.lit("FedEx")))
    .withColumn("is_lost", F.when(F.col("status") == "LOST", 1).otherwise(0))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("UNKNOWN")))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    latest
    .filter(F.col("carrier").isNotNull() & (F.col("shipped_date") > F.lit("2023-09-14").cast("date")))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        "shipped_date_plus_months",
        "rank_in_group",
        F.ceil("is_lost").alias("is_lost_whole"),
        F.round((F.datediff("shipped_date_plus_months", "shipped_date") / 30), 1).alias("months_shipped_date_to_shipped_date_plus_months"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled_2"),
    )
)

result = (
    valid
    .select("id", "carrier", "months_shipped_date_to_shipped_date_plus_months", "cost")
    .unpivot(
        ids=["id", "carrier"],
        values=["months_shipped_date_to_shipped_date_plus_months", "cost"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
