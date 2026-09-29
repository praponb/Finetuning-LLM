from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("rating_rounded", "id")
w4 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "TH", "US"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.round("rating", 2).alias("rating_rounded"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

result = (
    base
    .select(
        "id",
        "supplier_name",
        "city",
        "city_clean",
        F.first("rating", ignorenulls=True).over(w).alias("first_known_rating"),
        F.avg("rating").over(w2).alias("rating_avg_3"),
        F.ntile(10).over(w3).alias("rating_rounded_bucket"),
        F.sum("rating_rounded").over(w4).alias("running_rating_rounded"),
        F.sum("rating_rounded").over(w).alias("cumulative_rating_rounded"),
    )
)
