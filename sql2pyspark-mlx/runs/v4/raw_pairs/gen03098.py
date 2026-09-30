from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, -1)

base = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.concat("city", F.lit(", "), "country").alias("city_country"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

result = (
    base
    .select(
        "*",
        F.first("city", ignorenulls=True).over(w).alias("first_known_city"),
        F.avg("rating").over(w_grp).alias("rating_avg_3_prior"),
    )
)
