from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 2, 3, 5])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "PRIORITY").otherwise("GROUP_B").alias("city_zone"),
    )
)
