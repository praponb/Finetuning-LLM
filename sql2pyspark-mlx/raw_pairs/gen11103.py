from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("running_max_discount_pct")
w3 = Window.partitionBy(F.year("ends_on")).orderBy("ends_on", "id")
w4 = Window.partitionBy(F.year("ends_on")).orderBy("running_max_discount_pct_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_promotions
    .select(
        "id",
        "ends_on",
        F.min("discount_pct").over(w_grp).alias("running_min_discount_pct"),
        F.max("discount_pct").over(w_grp).alias("running_max_discount_pct"),
        F.count("discount_pct").over(w_grp).alias("running_count_discount_pct"),
    )
)

filtered = (
    metrics
    .filter((F.col("ends_on") > F.lit("2023-11-20").cast("date")) & F.month("ends_on").isin([6, 8, 12]))
    .withColumn("running_max_discount_pct_int", F.col("running_max_discount_pct").cast("int"))
)

scored = (
    filtered
    .filter(F.col("running_max_discount_pct_int") == 26)
    .select(
        "*",
        F.percent_rank().over(w_hist).alias("running_max_discount_pct_percent_rank"),
        F.lead("running_max_discount_pct_int", 2).over(w3).alias("next_running_max_discount_pct_int"),
        F.sum("running_max_discount_pct_int").over(w4).alias("cumulative_running_max_discount_pct_int"),
    )
)

filtered_promotions = (
    scored
    .filter((F.col("ends_on") > F.lit("2023-10-04").cast("date")) & (F.col("running_max_discount_pct") >= 0.24))
    .select(
        "id",
        "ends_on",
        "running_min_discount_pct",
        "running_max_discount_pct",
        "running_max_discount_pct_percent_rank",
        "next_running_max_discount_pct_int",
        "cumulative_running_max_discount_pct_int",
        F.when(F.col("cumulative_running_max_discount_pct_int") >= 437, "High").when(F.col("cumulative_running_max_discount_pct_int") >= 181, "Medium").otherwise("Low").alias("cumulative_running_max_discount_pct_int_band"),
    )
)

kept_keys = (
    filtered_promotions
    .select("id")
    .intersectAll(
        df_promotions
        .filter(F.col("channel") == "partner")
        .select("id")
    )
)

verified = (
    filtered_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "ends_on",
        "running_min_discount_pct",
        "running_max_discount_pct",
        "running_max_discount_pct_percent_rank",
        "cumulative_running_max_discount_pct_int",
    )
)
