from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.length("store_name") < 9) & ((F.col("sqft") == 8060) | (F.col("region") != "North")))
    .select(
        "city",
        "region",
        "sqft",
        F.coalesce("store_name", F.lit("unassigned")).alias("store_name_filled"),
    )
    .orderBy("store_name_filled", F.col("sqft").desc())
)
