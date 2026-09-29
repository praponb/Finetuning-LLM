from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_hist = Window.partitionBy("country", "city")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

result = (
    base
    .filter(F.col("city") == "Singapore")
    .select(
        "id",
        "rating_filled",
        "resolved_city",
        F.count("rating").over(w).alias("city_count_rating"),
        F.count("rating_filled").over(w_hist).alias("country_count_rating_filled"),
    )
)
