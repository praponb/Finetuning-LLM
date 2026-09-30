from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("years_since_last_restock").desc_nulls_last())

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-05-12")))
    .select(
        "id",
        "quantity",
        "last_restock",
        (F.datediff(F.current_date(), "last_restock") / 365.25).alias("years_since_last_restock"),
    )
)

valid = (
    base
    .filter(F.col("last_restock") >= F.lit("2023-06-03").cast("date"))
    .select(
        "*",
        (F.col("quantity") - F.col("years_since_last_restock")).alias("quantity_minus_years_since_last_restock"),
    )
)

matched_keys = (
    valid
    .select("id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity").between(106, 224))
        .select("id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("last_restock") >= F.lit("2023-08-20").cast("date"))
    .select(
        "quantity",
        "last_restock",
        "years_since_last_restock",
        "quantity_minus_years_since_last_restock",
        F.when(F.col("quantity_minus_years_since_last_restock") >= 65, "High").when(F.col("quantity_minus_years_since_last_restock") >= 32, "Medium").otherwise("Low").alias("quantity_minus_years_since_last_restock_band"),
    )
)

scored = cleaned.select("*", F.rank().over(w).alias("years_since_last_restock_rank"))

result = (
    scored
    .select("quantity", "last_restock", "quantity_minus_years_since_last_restock_band")
    .orderBy(F.col("quantity").desc_nulls_last(), F.col("last_restock").desc())
)
