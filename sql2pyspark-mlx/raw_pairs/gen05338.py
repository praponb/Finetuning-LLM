from pyspark.sql import functions as F

result = (
    df_promotions
    .select("promo_code", "id")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on").between("2023-10-01", "2024-10-01"))
        .select("promo_code", "id")
    )
)
