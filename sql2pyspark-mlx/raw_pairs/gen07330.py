from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("country").orderBy(F.col("city_length").desc(), "id")
w4 = Window.partitionBy("country").orderBy("city_length")
w5 = Window.partitionBy("country").orderBy(F.col("city_length").desc_nulls_last())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city") == "Tokyo"))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.length("city").alias("city_length"),
        F.lower("country").alias("country_lower"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

result = (
    base
    .filter(F.col("country").isin(["UK", "US"]))
    .select(
        "id",
        "supplier_name",
        "city_length",
        "city_clean",
        F.lag("rating", 2).over(w).alias("prev_rating"),
        F.sum("rating").over(w2).alias("country_sum_rating"),
        F.row_number().over(w3).alias("row_seq"),
        F.cume_dist().over(w4).alias("city_length_cume_dist"),
        F.percent_rank().over(w5).alias("city_length_percent_rank"),
    )
)
