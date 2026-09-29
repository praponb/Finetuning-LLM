from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last(), "id")
w2 = Window.partitionBy("country", "city").orderBy(F.col("dedup_rank").desc(), "id")
w3 = Window.orderBy(F.col("rating").desc())

base = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .withColumn("supplier_name_country_key", F.concat_ws("-", "supplier_name", "country"))
    .withColumn("country_lower", F.lower("country"))
    .withColumn("city_clean", F.coalesce("city", F.lit("DIRECT")))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    ranked
    .select("country", "city")
    .intersect(
        df_suppliers
        .filter(F.col("city").isin(["London", "New York"]))
        .select("country", "city")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "supplier_name",
        "rating",
        "country_lower",
        "city_clean",
        "dedup_rank",
        "row_num",
        F.percent_rank().over(w3).alias("rating_percent_rank"),
        F.dense_rank().over(w3).alias("rating_dense_rank"),
    )
)
