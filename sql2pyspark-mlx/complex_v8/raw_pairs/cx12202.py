from pyspark.sql import functions as F

base = (
    df_policies
    .filter((F.col("premium") > 1541.8) & (F.col("end_date").between(F.lit("2025-11-20").cast("date"), F.lit("2027-03-20").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
    )
)

combined_details_holder = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "p.holder_name",
    )
)

product_stats = (
    combined_details_holder
    .groupBy("product")
    .agg(F.avg("premium").alias("avg_premium"))
)

above_average = (
    combined_details_holder.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

result = (
    above_average
    .groupBy("end_date", "status")
    .pivot("product", ["TRAVEL", "AUTO"])
    .agg(F.max("avg_premium"))
    .orderBy("end_date", "status")
)
