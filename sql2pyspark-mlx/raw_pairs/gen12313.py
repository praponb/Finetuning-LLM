from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy("cost_int", "id")
w3 = Window.partitionBy("status").orderBy("cost_int", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("cost") >= 74)
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

filtered = (
    cleaned
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-08-14"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "cost",
        "status",
        "status_title",
        F.col("cost").cast("int").alias("cost_int"),
        F.lower("status").alias("status_lower"),
    )
)

with_windows = (
    filtered
    .filter(F.col("cost_int").isNull())
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.ntile(3).over(w2).alias("cost_int_bucket"),
        F.avg("cost").over(w3).alias("cost_avg_5_prior"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "cost", "status", "cost_share", "cost_int_bucket")
)

result = (
    latest
    .select(
        "id",
        "cost",
        "status",
        F.when(F.col("status").rlike("^(IN_TRANSIT|RETURNED)$"), "GROUP_B").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy("cost", F.col("status").desc())
)
