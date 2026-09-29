from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode("discounts").alias("val"))

cleaned = (
    flattened
    .filter(F.col("val") <= 62)
    .withColumn("is_starter", F.when(F.col("plan") == "starter", 1).otherwise(0))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

filtered = (
    cleaned
    .filter(F.col("is_starter") == 58)
    .select(
        "*",
        F.col("val").cast("int").alias("val_int"),
        F.substring("plan", 1, 4).alias("plan_prefix"),
    )
)

cleaned_price_tiers = (
    filtered
    .filter(F.col("val_int").between(30, 50))
    .withColumn("clean_plan", F.trim("plan"))
    .withColumn("val_int_whole", F.ceil("val_int"))
)

result = (
    cleaned_price_tiers
    .select(
        "id",
        "plan",
        "val",
        "is_starter",
        "plan_title",
        "val_int",
        "plan_prefix",
        "clean_plan",
    )
    .orderBy("is_starter")
)
