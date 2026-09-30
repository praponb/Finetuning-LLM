from pyspark.sql import functions as F

filtered_policys = df_policies.filter(F.col("holder_id").isNotNull())

labeled = (
    filtered_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.when(F.col("product").isin("TRAVEL", "LIFE"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
    )
)

labeled_2 = (
    labeled
    .filter(F.col("status_slug").isNotNull())
    .select(
        "*",
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
        F.initcap(F.trim(F.col("product_group"))).alias("product_group_clean"),
    )
)

result = (
    labeled_2
    .groupBy("end_date_dayofweek")
    .pivot("product_group", ["group_b", "other", "group_a"])
    .agg(F.min("premium"))
    .orderBy("end_date_dayofweek")
)
