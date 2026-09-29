from pyspark.sql import functions as F

valid = (
    df_warehouses
    .filter((F.col("capacity") < 11005) & (F.col("wh_code").isNotNull()))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
    )
)

scored = (
    valid
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.abs(F.col("capacity") - 11506).alias("capacity_distance"),
    )
)

scored_2 = (
    scored
    .filter(F.col("capacity") <= 11506)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.when(F.col("capacity_distance") >= 12000, "A").when(F.col("capacity_distance") >= 6000, "B").otherwise("C").alias("capacity_distance_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug"),
        F.when(F.col("capacity") >= 11005, "large").when(F.col("capacity") >= 4912, "medium").otherwise("small").alias("capacity_tier"),
    )
)

result = (
    scored_2
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "capacity_distance_tier",
        "wh_code_slug",
        "capacity_tier",
    )
    .orderBy(F.col("city").desc_nulls_first(), F.col("capacity").desc())
)
