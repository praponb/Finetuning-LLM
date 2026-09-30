from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_first())
w3 = Window.partitionBy("country", "city")

with_windows = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Tokyo"]))
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.cume_dist().over(w2).alias("rating_cume_dist"),
        F.count("rating").over(w3).alias("country_count_rating"),
    )
)

result = (
    with_windows
    .select("id", "supplier_name", "city", "country_count_rating")
    .orderBy(F.col("country_count_rating").desc())
)
