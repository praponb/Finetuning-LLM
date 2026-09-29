from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        F.first("carrier", ignorenulls=True).over(w).alias("first_known_carrier"),
        F.last("carrier", ignorenulls=True).over(w2).alias("last_known_carrier"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "status",
        "last_known_carrier",
        F.last("carrier", ignorenulls=True).over(w3).alias("last_known_carrier_2"),
    )
)

filtered = (
    scored
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST"]))
    .select("*", F.lower("status").alias("status_lower"))
)

filtered_shipments = (
    filtered
    .filter(F.col("status").isin(["LOST", "RETURNED", "DELIVERED"]))
    .withColumn("status_prefix", F.substring("status", 1, 2))
)

valid = (
    filtered_shipments
    .filter(F.col("status") == "RETURNED")
    .select(
        "id",
        "status",
        "last_known_carrier",
        "last_known_carrier_2",
        "status_lower",
        "status_prefix",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

stats = (
    valid
    .rollup("status")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
        F.grouping("status").alias("grouping_level"),
    )
)

result = (
    stats
    .select(
        "status",
        "num_status",
        F.when(F.col("status").rlike("^(DELIVERED|RETURNED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(IN_TRANSIT|LOST)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc(), F.col("status").desc())
)
