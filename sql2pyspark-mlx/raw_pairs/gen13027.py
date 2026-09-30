from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") == "TH"))
    .select(
        "*",
        F.lower("country").alias("country_lower"),
        F.when(F.col("country").isin(["US", "JP"]), "Priority").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

result = (
    base
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "country_lower",
        "country_group",
        "resolved_city",
        F.avg("rating").over(w).alias("rating_avg_5"),
        F.last("city", ignorenulls=True).over(w2).alias("last_known_city"),
    )
)
